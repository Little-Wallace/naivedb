mod types;

use crate::table::schema::DataSchemaRef;

pub use types::EncodeValue;

pub struct DataBlock {
    pub schema: DataSchemaRef,
    pub data: Vec<Vec<EncodeValue>>,
}

// TODO: replace it with async stream
pub type SendableDataBlockStream = Vec<DataBlock>;
