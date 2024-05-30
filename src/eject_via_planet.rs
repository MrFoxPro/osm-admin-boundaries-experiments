use std::{collections::HashMap, str::FromStr};

use geo::Geometry;
use geojson::de::deserialize_geometry;
use serde::Deserialize;

use crate::{eject, AdminRegion};

#[derive(Debug, Deserialize)]
pub struct PlanetExtractedAdminRegion {
    #[serde(deserialize_with = "deserialize_geometry")]
    pub geometry: Geometry<f64>,

    #[serde(flatten)]
    pub tags: HashMap<String, String>,

    pub name: String,
    pub admin_level: u8,
}

impl AdminRegion for PlanetExtractedAdminRegion {
    fn geometry(&self) -> &Geometry<f64> {
        &self.geometry
    }
    fn name(&self) -> &String {
        &self.name
    }
    fn admin_level(&self) -> u8 {
        self.admin_level
    }
    fn tags(&self) -> &HashMap<String, String> {
        &self.tags
    }
}

pub fn load_planet_extracted_admin_places() -> anyhow::Result<Vec<PlanetExtractedAdminRegion>> {
    let files = std::fs::read_dir("./extracted/al234.extract_full2")?
        .filter(|f| f.as_ref().unwrap().path().extension().unwrap() == "geojson")
        .collect::<Vec<_>>();

    let mut output = Vec::with_capacity(files.len());
    for file in files.into_iter() {
        let file = file?;
        let value = serde_json::from_str::<serde_json::Value>(&std::fs::read_to_string(file.path())?);

        let Ok(value) = value else {
            eprintln!("failed to parse file {:#?}: {}", file.path(), value.err().unwrap());
            continue;
        };

        let geometry = geojson::Geometry::from_json_value(value["geometry"].clone())?;
        let tags = serde_json::from_value::<HashMap<String, String>>(value["properties"].clone())?;

        let Some(name) = tags.get("name") else {
            eprintln!("no name in {:#?}", file.path());
            continue;
        };
        let Some(admin_level) = tags.get("admin_level").and_then(|s| u8::from_str(s).ok()) else {
            eprintln!("no admin_level in {:#?}", file.path());
            continue;
        };

        output.push(PlanetExtractedAdminRegion {
            name: name.to_string(),
            admin_level,
            geometry: geometry.try_into()?,
            tags,
        });
    }

    Ok(output)
}