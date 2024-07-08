// fn check_random_thing() -> anyhow::Result<()> {
//     let raw = String::from_utf8(std::fs::read("/home/foxpro/misc/osm/al2.geom.geojson")?)?;
//     let gj = geojson::GeoJson::from_str(&raw)?;

//     let fc = match gj {
//         geojson::GeoJson::FeatureCollection(f) => f,
//         _ => todo!(),
//     };

//     if fc.features.is_empty() {
//         println!("skip fc {fc}");
//     }
//     for c in fc.features.iter() {
//         let g: Geometry<f64> = c.geometry.clone().unwrap().try_into()?;
//         let oxford = Point::<f64>::new(-1.2578499, 51.7520131);
//         if g.contains(&oxford) {
//             println!("place containing oxford: {}", c.properties.clone().unwrap()["tags"]["name"]);
//         }
//     }
//     Ok(())
// }