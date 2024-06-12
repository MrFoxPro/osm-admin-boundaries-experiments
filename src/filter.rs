use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::available_parallelism;

pub fn filter(input: &str, output: &str) {
    let reader = Reader::new(&PathBuf::from(input)).unwrap();

    let mut file_info = FileInfo::default();
    file_info.with_writingprogram_str("pbf-io-example");
    let mut writer =
        Writer::from_file_info(PathBuf::from(output), file_info, CompressionType::Zlib).unwrap();
    writer.write_header().unwrap();

    let writer = Arc::new(Mutex::new(writer));
    reader
        .parallel_for_each(available_parallelism().unwrap().into(), move |element| {
            let mut preserve = false;
            match element {
                Element::Relation { ref relation } => {}
                Element::Node { ref node } => {
                    for tag1 in node.tags().iter().rev() {
                        let (k, v) = (tag1.k(), tag1.v());
                        const PLACES: [&str; 7] = [
                            "country", "state", "region", "county", "city", "town", "village",
                        ];
                        if k == "place" && PLACES.contains(&v.as_ref()) {
                            preserve = true
                        }
                    }
                }
                Element::Way { ref way } => {
                    preserve = way.tags().iter().any(|t| {
                        [("boundary", "administrative"), ("natural", "coastline")].contains(&(t.k().as_str(), t.v().as_str()))
                    });
                }
                _ => {}
            }

            if preserve {
                writer
                    .lock()
                    .unwrap()
                    .write_element(element.clone())
                    .unwrap();
            }

            Ok(())
        })
        .unwrap();
}
