use std::str::FromStr;

use geo::{Contains, Geometry, Point};

fn combine_extracts() -> anyhow::Result<()> {
    let files = std::fs::read_dir("/home/foxpro/misc/osm/al234.extract_full")?
        .filter(|p| p.as_ref().unwrap().path().extension().unwrap() == "geojson")
        .map(Result::unwrap)
        .collect::<Vec<_>>();

    let mut items = Vec::with_capacity(files.len());
    for f in files.iter() {
        let s = std::fs::read_to_string(f.path())?;
        let v = serde_json::Value::from_str(&s);
        if let Ok(v) = v {
            items.push(v);
        } else {
            eprintln!("failed to parse file: {:#?}", f.path());
        }
    }
    std::fs::write("al234.extract.json", serde_json::to_string(&items)?)?;
    Ok(())
}

fn check_random_thing() -> anyhow::Result<()> {
    let raw = String::from_utf8(std::fs::read("/home/foxpro/misc/osm/al2.geom.geojson")?)?;
    let gj = geojson::GeoJson::from_str(&raw)?;

    let fc = match gj {
        geojson::GeoJson::FeatureCollection(f) => f,
        _ => todo!(),
    };

    if fc.features.is_empty() {
        println!("skip fc {fc}");
    }
    for c in fc.features.iter() {
        let g: Geometry<f64> = c.geometry.clone().unwrap().try_into()?;
        let oxford = Point::<f64>::new(-1.2578499, 51.7520131);
        if g.contains(&oxford) {
            println!("place containing oxford: {}", c.properties.clone().unwrap()["tags"]["name"]);
        }
    }
    Ok(())
}