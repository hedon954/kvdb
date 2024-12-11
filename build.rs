use std::fs;

fn main() {
    fs::create_dir_all("src/pb").unwrap();
    let mut config = prost_build::Config::new();
    config.bytes(&["."]);
    config.type_attribute(".", "#[derive(PartialOrd)]");
    config
        .out_dir("src/pb")
        .compile_protos(&["protos/abi.proto"], &["protos"])
        .unwrap();
}
