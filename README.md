# Merkle Tree–Based Dataset Integrity Verification

This project implements a Merkle Tree–based system to verify the integrity of large-scale JSON datasets.  
It detects data tampering (modification, deletion, insertion), supports partial Merkle root recomputation,
generates cryptographic proofs, and evaluates performance on large datasets.

The implementation is designed to work efficiently with datasets containing up to 1.5 million records.

---

## Features

- SHA-256–based hashing of dataset records
- Merkle Tree construction for large-scale datasets
- Detection of data tampering (modify, delete, insert)
- Partial Merkle root recomputation after tampering
- Merkle proof generation and verification
- Performance analysis (hashing speed, build time, memory usage)
- Interactive menu-driven CLI
- Streamlit-based interface for visualization

---

## Dataset

This project uses the *Amazon Movies & TV Reviews dataset (JSON format)*.

- File name expected by the program: `Movies_and_TV_5.json`
- Dataset size: up to 1.5 million records
- Due to GitHub file size limitations, the dataset is *not included* in this repository.

You can download the dataset from:  
https://nijianmo.github.io/amazon/index.html

After downloading, place the file in the project root directory before running the program.
