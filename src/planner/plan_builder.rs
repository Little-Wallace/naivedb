use crate::common::EncodeValue;
use crate::errors::{MySQLError, MySQLResult};
use crate::planner::point_get_plan::QueryPlanBuilder;
use crate::planner::{CreateTablePlan, InsertPlan, PlanNode};
use crate::session::SessionRef;
use crate::table::schema::{DataSchema, TableInfo};
use sqlparser::ast::{
    ColumnDef, Expr, Ident, ObjectName, Query, SqlOption, Statement, TableConstraint,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;

pub struct PlanBuilder {
    session: SessionRef,
}

impl PlanBuilder {
    pub fn create(session: SessionRef) -> Self {
        Self { session }
    }

    pub fn build_from_sql(&self, query: &str) -> MySQLResult<PlanNode> {
        let dialect = MySqlDialect {};
        println!("build_from_sql");
        let statement = Parser::parse_sql(&dialect, query)?;
        println!("build_from_sql get {}", statement.len());
        if statement.len() != 1 {
            return Result::Err(MySQLError::UnsupportSQL);
        }
        statement
            .first()
            .map(|s| self.statement_to_plan(&s))
            .unwrap()
    }

    pub fn statement_to_plan(&self, statement: &Statement) -> MySQLResult<PlanNode> {
        println!("{:?}", statement);
        match statement {
            Statement::Query(q) => self.sql_query_to_plan(q),
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => self.sql_insert_to_plan(table_name, columns, source),
            Statement::CreateTable {
                name,
                columns,
                constraints,
                without_rowid,
                or_replace,
                table_properties,
                ..
            } => self.sql_create_table_to_plan(
                name,
                columns,
                constraints,
                *without_rowid,
                *or_replace,
                table_properties,
            ),
            _ => return Err(MySQLError::UnsupportSQL),
        }
    }

    fn sql_query_to_plan(&self, query: &Box<Query>) -> MySQLResult<PlanNode> {
        let point_get_builder = QueryPlanBuilder::new(self.session.clone());
        if let Some(plan) = point_get_builder.try_point_get(query)? {
            return Ok(PlanNode::PointGet(plan));
        }
        unimplemented!()
    }

    fn sql_create_table_to_plan(
        &self,
        name: &ObjectName,
        column_defs: &Vec<ColumnDef>,
        constrains: &Vec<TableConstraint>,
        _without_rowid: bool,
        _or_replace: bool,
        _table_properties: &Vec<SqlOption>,
    ) -> MySQLResult<PlanNode> {
        let table_info = Arc::new(TableInfo::create(name, column_defs, constrains)?);
        Ok(PlanNode::CreateTable(CreateTablePlan { table_info }))
    }

    fn sql_insert_to_plan(
        &self,
        table_name: &ObjectName,
        cols: &[Ident],
        source: &Query,
    ) -> MySQLResult<PlanNode> {
        let table_name = table_name.0.last().unwrap().value.to_lowercase();
        let table = match self.session.lock().unwrap().get_table(&table_name) {
            Some(t) => t,
            None => return Err(MySQLError::NoTable),
        };
        let mut columns = vec![];
        for col_name in cols {
            let col_name = col_name.value.to_lowercase();
            let col = match table.get_column(&col_name) {
                Some(col) => col,
                None => return Err(MySQLError::NoColumn),
            };
            columns.push(col);
        }
        match &source.body {
            sqlparser::ast::SetExpr::Values(values) => {
                let mut ec_values = vec![];
                for value in values.0.iter() {
                    if value.len() != cols.len() {
                        return Err(MySQLError::ColumnMissMatch);
                    }
                    let mut row = vec![];
                    for e in value.iter() {
                        match e {
                            Expr::Value(v) => {
                                row.push(EncodeValue::from_parse_value(v.clone())?);
                            }
                            _ => return Err(MySQLError::UnsupportSQL),
                        }
                    }
                    ec_values.push(row);
                }
                Ok(PlanNode::Insert(InsertPlan {
                    table,
                    values: ec_values,
                    schema: DataSchema { columns },
                    session: self.session.clone(),
                }))
            }
            _ => Err(MySQLError::UnsupportSQL),
        }
    }
}
