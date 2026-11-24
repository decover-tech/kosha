use std::path::PathBuf;
use std::sync::Arc;

use serde::Deserialize;
use tokenizers::{PaddingParams, PaddingStrategy, PostProcessor, Tokenizer, TruncationParams};
use tract_onnx::prelude::*;

use super::{EmbedError, Embedder, EmbeddingSpec};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ModelConfig {
    model_path: PathBuf,
    tokenizer_path: PathBuf,
    sequence_length: usize,
}

enum Input {
    Ids,
    AttentionMask,
    TypeIds,
}

pub(super) struct OnnxEmbedder {
    model: String,
    dims: usize,
    sequence_length: usize,
    tokenizer: Tokenizer,
    inputs: Vec<Input>,
    plan: Arc<TypedRunnableModel>,
}

impl OnnxEmbedder {
    pub(super) fn load(spec: &EmbeddingSpec, config: &ModelConfig) -> Result<Self, EmbedError> {
        let not_configured = |reason: &dyn std::fmt::Display| {
            EmbedError::NotConfigured(format!("ONNX embedding model {}: {reason}", spec.model))
        };
        let sequence_length = config.sequence_length;
        if !(1..=512).contains(&sequence_length) {
            return Err(not_configured(&"sequence_length must be in 1..=512"));
        }
        let mut tokenizer =
            Tokenizer::from_file(&config.tokenizer_path).map_err(|e| not_configured(&e))?;
        if tokenizer
            .get_post_processor()
            .is_some_and(|p| p.added_tokens(false) >= sequence_length)
        {
            return Err(not_configured(&"sequence_length leaves no room for text"));
        }
        let truncation = TruncationParams {
            max_length: sequence_length,
            ..tokenizer.get_truncation().cloned().unwrap_or_default()
        };
        tokenizer
            .with_truncation(Some(truncation))
            .map_err(|e| not_configured(&e))?;
        let mut padding = match tokenizer.get_padding() {
            Some(params) => params.clone(),
            None => {
                let pad_id = tokenizer.token_to_id("[PAD]").ok_or_else(|| {
                    not_configured(&"tokenizer must declare padding or have a [PAD] token")
                })?;
                PaddingParams {
                    pad_id,
                    ..PaddingParams::default()
                }
            }
        };
        padding.strategy = PaddingStrategy::Fixed(sequence_length);
        padding.pad_to_multiple_of = None;
        tokenizer.with_padding(Some(padding));

        let mut model = tract_onnx::onnx()
            .model_for_path(&config.model_path)
            .map_err(|e| not_configured(&e))?;
        let inputs = model
            .input_outlets()
            .map_err(|e| not_configured(&e))?
            .iter()
            .map(|outlet| match model.node(outlet.node).name.as_str() {
                "input_ids" => Ok(Input::Ids),
                "attention_mask" => Ok(Input::AttentionMask),
                "token_type_ids" => Ok(Input::TypeIds),
                name => Err(not_configured(&format_args!("unsupported input {name:?}"))),
            })
            .collect::<Result<Vec<_>, _>>()?;
        if !inputs.iter().any(|i| matches!(i, Input::Ids))
            || !inputs.iter().any(|i| matches!(i, Input::AttentionMask))
        {
            return Err(not_configured(
                &"model needs input_ids and attention_mask inputs",
            ));
        }
        for i in 0..inputs.len() {
            model
                .set_input_fact(
                    i,
                    InferenceFact::dt_shape(i64::datum_type(), tvec!(1, sequence_length)),
                )
                .map_err(|e| not_configured(&e))?;
        }
        let dims = spec.dims as usize;
        let model = model.into_optimized().map_err(|e| not_configured(&e))?;
        let outputs = model.output_outlets().map_err(|e| not_configured(&e))?;
        if outputs.len() != 1
            || !model.output_fact(0).is_ok_and(|fact| {
                fact.datum_type == f32::datum_type() && fact.shape.as_concrete() == Some(&[1, dims])
            })
        {
            return Err(not_configured(&format_args!(
                "model must output a single float32 tensor of shape [1, {dims}]"
            )));
        }
        Ok(Self {
            model: spec.model.clone(),
            dims,
            sequence_length,
            tokenizer,
            inputs,
            plan: model.into_runnable().map_err(|e| not_configured(&e))?,
        })
    }

    fn embed_one(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        let failed = |stage| {
            EmbedError::Provider(format!(
                "ONNX embedding model {}: {stage} failed",
                self.model
            ))
        };
        let encoding = self
            .tokenizer
            .encode(text, true)
            .map_err(|_| failed("tokenization"))?;
        let mut inputs = TVec::with_capacity(self.inputs.len());
        for input in &self.inputs {
            let values = match input {
                Input::Ids => encoding.get_ids(),
                Input::AttentionMask => encoding.get_attention_mask(),
                Input::TypeIds => encoding.get_type_ids(),
            };
            let values: Vec<_> = values.iter().copied().map(i64::from).collect();
            let tensor = Tensor::from_shape(&[1, self.sequence_length], &values)
                .map_err(|_| failed("input preparation"))?;
            inputs.push(tensor.into_tvalue());
        }
        let outputs = self.plan.run(inputs).map_err(|_| failed("inference"))?;
        let vector = outputs[0]
            .to_plain_array_view::<f32>()
            .map_err(|_| failed("output decoding"))?;
        Ok(vector.iter().copied().collect())
    }
}

impl Embedder for OnnxEmbedder {
    fn model_id(&self) -> &str {
        &self.model
    }

    fn dims(&self) -> usize {
        self.dims
    }

    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        texts.iter().map(|text| self.embed_one(text)).collect()
    }
}
