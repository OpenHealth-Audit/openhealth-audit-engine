# 🏥 OpenHealth Audit Engine

![License](https://img.shields.io/badge/License-MIT-blue.svg)
![Build Status](https://img.shields.io/badge/Data_Pipeline-Automated_GitOps-success.svg)
![Architecture](https://img.shields.io/badge/Architecture-Hybrid_Edge--Cloud-orange.svg)

An open-source, Zero-Knowledge medical billing transparency engine. This application allows American patients to algorithmically audit their itemized hospital bills, mathematically calculate facility markups, and cross-reference live federal Medicare baselines without transmitting Protected Health Information (PHI) to centralized servers.

## 🚀 The Architecture

U.S. healthcare data is heavily fragmented. While the CMS Physician Fee Schedule (PFS) offers live APIs, Clinical Labs (CLFS) and Pharmacy Drugs (ASP) are locked inside schema-shifting Excel files and AMA license walls. 

To solve this data monopoly, OpenHealth operates on a **Hybrid Federated Edge-Cloud Architecture**:

1. **Zero-Knowledge Edge Compute:** Uses WebAssembly and `Tesseract.js` to perform Optical Character Recognition (OCR) entirely within the user's browser memory. Your medical bill never leaves your device.
2. **Boundary-Agnostic Contextual NLP:** Parses fractured OCR text, algorithmically isolates 5-digit CPT/HCPCS medical codes, rescues misaligned monetary values, and filters out geographic noise (Zip Codes/PO Boxes).
3. **High-Availability Proxy Waterfall:** Bypasses CORS restrictions to query live U.S. government endpoints (CMS PFS) in real-time.
4. **GitOps Omni-Scraper (The Data Lake):** A fully automated Python CI/CD pipeline runs monthly via GitHub Actions. It utilizes Deep Tree Navigation to bypass government license walls, extracts fragmented data directly into RAM, normalizes schema drift, and publishes a highly compressed JSON Data Lake to a free global CDN.

## 🛠️ Installation & Local Development

To run the Audit Engine locally:

1. Clone the repository:
```bash
git clone [https://github.com/YOUR_ORG_NAME/openhealth-audit-engine.git](https://github.com/YOUR_ORG_NAME/openhealth-audit-engine.git)
cd openhealth-audit-engine
```

2. Generate the local Data Lake (Requires Python 3.10+ and Pandas):
```bash
pip install pandas requests beautifulsoup4
python3 update_data.py
```

3. Start a local server to bypass browser CORS policies:
```bash
python3 -m http.server 8000
```

4. Open your browser and navigate to `http://localhost:8000`.

## 🤖 Automated GitOps Pipeline
This repository is completely self-healing. The `.github/workflows/data-pipeline.yml` file is configured to run the `update_data.py` Omni-Scraper on the 1st of every month. It automatically scrapes CMS.gov, merges the newest drug and lab schedules, and commits the updated `supplemental.json` back to the main branch. 

## ⚖️ Disclaimer
*This application is an educational transparency tool designed to calculate federal baseline variants (Quantum Meruit). It does not constitute legal or financial advice. Hospitals are not federally mandated to match Medicare prices, but this data empowers patients during financial negotiations and dispute resolutions.*
