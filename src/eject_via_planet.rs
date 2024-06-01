use std::{collections::HashMap, str::FromStr};

use geo::Geometry;
use geojson::de::deserialize_geometry;
use serde::Deserialize;

use crate::{eject, AdminRegion};

#[derive(Debug, Clone, Deserialize)]
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

pub fn load_planet_extracted_places() -> anyhow::Result<Vec<PlanetExtractedAdminRegion>> {
    let str = std::fs::read_to_string("./extracted/places.geojson")?;
    let geojson = geojson::FeatureCollection::from_str(&str)?;

    let mut output = Vec::with_capacity(geojson.features.len());

    for feature in geojson.features {
        let properties: HashMap<String, String> =
            serde_json::from_value(serde_json::to_value(feature.properties.as_ref().unwrap())?)?;

        let Some(admin_level) = properties
            .get("admin_level")
            .and_then(|s| u8::from_str(s).ok())
        else {
            eprintln!("no admin_level in {:#?}", feature);
            continue;
        };

        let Some(name) = properties.get("name").map(ToOwned::to_owned) else {
            eprintln!("no name in {:#?}", feature);
            continue;
        };

        output.push(PlanetExtractedAdminRegion {
            name,
            admin_level,
            geometry: feature.geometry.unwrap().try_into()?,
            tags: properties,
        });
    }

    Ok(output)
}
