extern crate cbindgen;

use std::env;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let bin_dir = env::var("FalkorDBRS_BINDIR").unwrap_or(".".to_string());

    cbindgen::Builder::new()
      .with_crate(crate_dir)
      .with_language(cbindgen::Language::C)
      .with_pragma_once(true)
      .generate()
      .expect("Unable to generate bindings")
      .write_to_file(format!("{bin_dir}/FalkorDBRS.h"));
}