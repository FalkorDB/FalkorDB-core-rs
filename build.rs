use std::error::Error;

fn generate_binding(
    root_folder: &str,
    header_relative_path: &str,
) -> Result<(), Box<dyn Error>> {
    let bindings = bindgen::builder()
        .header(format!("{root_folder}/headers/{header_relative_path}.h"))
        .derive_copy(false)
        .generate()?;

    bindings.write_to_file(format!(
        "{root_folder}/src/binding/{header_relative_path}.rs"
    ))?;

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let root_folder = std::env::var("CARGO_MANIFEST_DIR")?;

    for header in ["query_ctx_struct"].into_iter() {
        generate_binding(&root_folder, header)?;
    }

    Ok(())
}
