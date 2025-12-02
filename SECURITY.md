# Security Policy

## Core Philosophy

Wrought Iron is built on a **"Secure by Design"** philosophy. Our primary security guarantees are:
1.  **Zero Egress:** The application must never initiate an outbound network request.
2.  **Deterministic Forensics:** Every data mutation must be logged in the immutable audit trail.
3.  **Local Execution:** All processing occurs on the local metal; no cloud dependencies.

We take any breach of these three pillars as a critical incident.

## Supported Versions

We currently support the following versions of Wrought Iron with security updates and hotfixes.

| Version | Supported          | Notes |
| ------- | ------------------ | ----- |
| 1.0.x   | :white_check_mark: | Current Air-Gapped Release |
| < 1.0   | :x:                | Deprecated / Alpha |

## Reporting a Vulnerability

If you discover a security vulnerability—especially one that compromises the **Air-Gap Guarantee** or the **Audit Log Integrity**—please follow the strict protocol below.

**⚠️ DO NOT open a public GitHub issue for security vulnerabilities.**

### How to Report
Please report sensitive findings via email to the core maintainers.

* **Email:** security@wrought-iron.local
* **Subject:** `[SECURITY] Vulnerability Report - [Component]`

### Critical Vectors
We prioritize vulnerabilities that affect the following critical subsystems:
* **Network Leaks:** Any instance where `wi` attempts to contact an external IP or DNS.
* **Audit Bypass:** Methods to modify SQLite data using `wi` commands without generating a `_wi_audit_log_` entry.
* **Crypto Failure:** Weaknesses in the AES-256 implementation used by `wi connect encrypt`.
* **PII Leakage:** Failure of the Presidio scanner to detect standard PII entities when configured.

### Response Timeline
* **Acknowledgment:** We will acknowledge receipt of your report within 48 hours.
* **Triage:** We aim to validate the vulnerability within 5 business days.
* **Resolution:** Critical patches (especially for Data Egress issues) are prioritized above all feature work.

### Disclosure Policy
Due to the sensitive nature of the environments where Wrought Iron is deployed (Defense, Healthcare), we strictly adhere to a **Responsible Disclosure** policy. We ask that you do not disclose the vulnerability publicly until a patch has been released and verified.
