use crate::common::EncodeValue;

pub enum Expression {
    Eq {
        left: Box<Expression>,
        right: Box<Expression>,
    },
    Column(String),
    Value(EncodeValue),
}
