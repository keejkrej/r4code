use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelOption {
    pub id: String,
    pub label: String,
    #[serde(rename = "type")]
    pub option_type: String, // "select" or "boolean"
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Model {
    pub id: String,
    pub label: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<ModelOption>,
}

pub fn default_model(models: &[Model]) -> Model {
    for model in models {
        if model.id == "default" {
            return model.clone();
        }
    }
    if let Some(first) = models.first() {
        return first.clone();
    }
    Model {
        id: "default".to_string(),
        label: "Default".to_string(),
        options: Vec::new(),
    }
}
