pub struct FileMeta {
    pub id: String,
    pub path: String,
    pub duration_in_sec: f32,
}

pub struct File {
    pub id: String,
    pub type_: String,
    pub data: Vec<u8>,
}
