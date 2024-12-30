use certify::{generate_ca, generate_cert, CertType, CA};
use tokio::fs;

struct CertPem {
    cert_type: CertType,
    cert: String,
    key: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let pem = create_ca()?;
    gen_files(&pem).await?;

    let ca = CA::load(&pem.cert, &pem.key)?;
    let pem = create_cert(&ca, &["kvserver.acme.inc"], false)?;
    gen_files(&pem).await?;
    let pem = create_cert(&ca, &["awesome-device-id"], true)?;
    gen_files(&pem).await?;
    Ok(())
}

fn create_ca() -> anyhow::Result<CertPem> {
    let (cert, key) = generate_ca(
        "CN",
        "Acme Inc.",
        "Acme CA",
        certify::CertSigAlgo::ED25519,
        None,
        Some(10 * 365),
    )?;
    Ok(CertPem {
        cert_type: CertType::CA,
        cert,
        key,
    })
}

fn create_cert(ca: &CA, domains: &[&str], is_client: bool) -> anyhow::Result<CertPem> {
    let (days, cert_type) = if is_client {
        (Some(365), CertType::Client)
    } else {
        (Some(5 * 365), CertType::Server)
    };
    let (cert, key) = generate_cert(
        ca,
        domains.to_vec(),
        "CN",
        "Acme Inc.",
        "Acme CA",
        certify::CertSigAlgo::ED25519,
        None,
        is_client,
        days,
    )?;
    Ok(CertPem {
        cert_type,
        cert,
        key,
    })
}

async fn gen_files(pem: &CertPem) -> anyhow::Result<()> {
    let name = match pem.cert_type {
        CertType::CA => "ca",
        CertType::Client => "client",
        CertType::Server => "server",
    };
    fs::write(format!("fixtures/{}.cert", name), pem.cert.as_bytes()).await?;
    fs::write(format!("fixtures/{}.key", name), pem.key.as_bytes()).await?;
    Ok(())
}
