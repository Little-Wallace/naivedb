pub enum Expr {
    Equal { left: Box<Expr>, right: Box<Expr> },
    ColumnRef { column_name: String },
    Literal,
}
