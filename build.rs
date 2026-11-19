// protoc 없이 빌드하려고 순수 Rust 컴파일러(protox)로 디스크립터를 만들어 prost-build에 넘긴다.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/feed.proto");
    let descriptors = protox::compile(["proto/feed.proto"], ["proto"])?;
    prost_build::compile_fds(descriptors)?;
    Ok(())
}
