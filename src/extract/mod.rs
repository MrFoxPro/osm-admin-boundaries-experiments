mod converter;
mod find_relations;
use std::{str::FromStr, time::Instant};

use converter::{convert, Polygon};
use find_relations::read_ways_and_relation;
use geo::{Coord, LineString};
use geojson::{Feature, FeatureCollection};

pub fn extract_polygons(input_pbf: &str, output_base_dir: &str) -> anyhow::Result<()> {
    let relations = read_ways_and_relation(input_pbf, 2, 4, false);
    let polygons = convert(relations);

    let output_base_dir = std::path::PathBuf::from_str(output_base_dir)?;
    std::fs::create_dir_all(&output_base_dir)?;

    let mut fc = FeatureCollection {
        bbox: None,
        features: Vec::with_capacity(polygons.len()),
        foreign_members: None,
    };

    println!("writing output files...");
    for polygon in polygons.iter() {
        let properties = geojson::JsonObject::from_iter([
            ("name".to_owned(), serde_json::to_value(&polygon.name)?),
            (
                "admin_level".to_owned(),
                serde_json::to_value(&polygon.admin_level)?,
            ),
        ]);

        let geo_polygons = polygon
            .points
            .iter()
            .map(|points| {
                geo::Polygon::new(
                    LineString(
                        points
                            .iter()
                            .map(|p| Coord { x: p.lon, y: p.lat })
                            .collect(),
                    ),
                    vec![],
                )
            })
            .collect::<Vec<_>>();

        if geo_polygons.is_empty() {
            eprintln!("empty polygon {}:{}. Number of points: {}", polygon.name, polygon.relation_id, polygon.points.len());
            continue;
        }

        let geometry = geojson::Geometry::new(match geo_polygons.len() {
            1 => geojson::Value::from(&geo_polygons[0]),
            _ => geojson::Value::from(&geo::MultiPolygon(geo_polygons)),
        });

        let feature = Feature {
            id: Some(geojson::feature::Id::Number(polygon.relation_id.into())),
            geometry: Some(geometry),
            properties: Some(properties),
            bbox: None,
            foreign_members: None,
        };

        let mut safe_name = polygon.name.clone();
        safe_name.retain(|c| !r#"\\/&:<>|*"#.contains(c));
        let file_name = format!("{}-{}.geojson", safe_name, polygon.relation_id);

        std::fs::write(output_base_dir.join(&file_name), feature.to_string())?;

        fc.features.push(feature);
    }

    std::fs::write(output_base_dir.join("ALL.geojson"), serde_json::to_string(&fc)?)?;

    Ok(())
}
