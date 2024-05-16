use super::cell::CellValue;


#[derive(Debug, Clone)]
pub struct Condition {
    pub target_column: String,
    pub conditional: Conditional,
    pub value: CellValue,
    pub relational: Option<Relation>// relation to next condition in collection
}

#[derive(Debug, Clone)]
pub enum Conditional {
    NotEqual,
    Equal,
    Greater,
    Smaller,
    EqualGreater,
    EqualSmaller,
    All,
}

impl Conditional {
    pub fn parse(a: String) -> Result<Self, String> {
        match &a[..] {
            "!=" => Ok(Conditional::NotEqual),
            "==" => Ok(Conditional::Equal),
            ">=" => Ok(Conditional::Greater),
            "<=" => Ok(Conditional::Smaller),
            ">"  => Ok(Conditional::EqualGreater),
            "<"  => Ok(Conditional::EqualSmaller),
            "*"  => Ok(Conditional::All),
            _ => Err("condition pattern not recognised".to_owned())
        }
    }
}

#[derive(Debug, Clone)]
pub enum Relation {
    AND,
    OR,
    NOT,
}

impl Relation {
    pub fn parse(a: String) -> Result<Self, String> {
        match &a[..] {
            "AND"|"&&" => Ok(Relation::AND),
            "OR"|"||" => Ok(Relation::OR),
            "NOT"|"!!" => Ok(Relation::NOT),
            _ => Err("".to_owned()),
        }
    }
}