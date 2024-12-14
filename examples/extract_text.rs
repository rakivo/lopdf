use std::fs::File;
use std::path::Path;
use std::fmt::Debug;
use std::time::Instant;
use std::sync::{Arc, Mutex};
use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Write};

use lopdf::{Document, Object};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

static IGNORE: &[&[u8]] = &[
    b"Length",
    b"BBox",
    b"FormType",
    b"Matrix",
    b"Type",
    b"XObject",
    b"Subtype",
    b"Filter",
    b"ColorSpace",
    b"Width",
    b"Height",
    b"BitsPerComponent",
    b"Length1",
    b"Length2",
    b"Length3",
    b"PTEX.FileName",
    b"PTEX.PageNumber",
    b"PTEX.InfoDict",
    b"FontDescriptor",
    b"ExtGState",
    b"MediaBox",
    b"Annot",
];

struct PdfText {
    text: BTreeMap::<u32, Vec::<String>>,
    errors: Vec::<String>
}

fn filter_func(object_id: (u32, u16), object: &mut Object) -> Option::<((u32, u16), Object)> {
    if IGNORE.contains(&object.type_name().unwrap_or_default().as_bytes()) {
        return None;
    }
    if let Ok(d) = object.as_dict_mut() {
        d.remove(b"Producer");
        d.remove(b"ModDate");
        d.remove(b"Creator");
        d.remove(b"ProcSet");
        d.remove(b"Procset");
        d.remove(b"XObject");
        d.remove(b"MediaBox");
        d.remove(b"Annots");
        if d.is_empty() {
            return None;
        }
    }
    Some((object_id, object.to_owned()))
}

fn load_pdf<P: AsRef<Path>>(path: P) -> Result::<Document, Error> {
    Document::load_filtered(path, filter_func).map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
}

fn get_pdf_text(doc: &Document) -> Result::<PdfText, Error> {
    let mut pdf_text = PdfText {
        text: BTreeMap::new(),
        errors: Vec::new()
    };

    let pdf_text_am = Arc::new(Mutex::new(&mut pdf_text));

    doc.get_pages()
        .into_par_iter()
        .map(|(npage, page_id)| {
            let text = doc.extract_text(&[npage]).map_err(|e| {
                Error::new(ErrorKind::Other,
                           format!("could not to extract text from page {npage} id={page_id:?}: {e:}"))
            })?;

            Ok((npage,
                text.split('\n')
                    .map(|s| s.to_lowercase())
                    .collect()))
        }).for_each(|page: std::io::Result::<_>| {
            let mut pdf_text = unsafe { pdf_text_am.lock().unwrap_unchecked() };
            match page {
                Ok((npage, lines)) => { pdf_text.text.insert(npage, lines); },
                Err(e) => pdf_text.errors.push(e.to_string()),
            }
        });

    Ok(pdf_text)
}

fn pdf2text<P: AsRef<Path> + Debug>(path: P, output: P) -> Result<(), Error> {
    println!("Load {path:?}");
    let doc = load_pdf(&path)?;
    let text = get_pdf_text(&doc)?;
    if !text.errors.is_empty() {
        eprintln!("{path:?} has {} errors:", text.errors.len());
        for error in &text.errors[..10] {
            eprintln!("{error:?}");
        }
    }
    println!("Write {output:?}");
    let mut f = File::create(output)?;
    f.write_all(text.text.iter()
                .map(|(_, text)| text.join(" "))
                .collect::<String>()
                .as_bytes())?;
    Ok(())
}

fn main() -> Result::<(), Error> {
    let args = std::env::args().collect::<Vec::<_>>();
    if args.len() < 3 {
        panic!("usage: ./{program} <file_path.pdf> <output.txt>", program = args[0])
    }

    let start_time = Instant::now();
    let ref pdf_path = args[1];
    let ref output_path = args[2];
    pdf2text(&pdf_path, &output_path)?;
    println!{
        "Done after {:.1} seconds.",
        Instant::now().duration_since(start_time).as_secs_f64()
    };
    Ok(())
}
