Use cfssl to sign, verify and bundle TLS certificates and output results as
JSON.

Use cfssljson to take that JSON output and split them into separate key,
certificate, CSR and bundle files.

The ca-csr.json file contains general config information about our Certificate
Authority (CA).

The ca-config.json file defines the CA's signing policy.
The two profiles in this file indicate that the CA can generate server and
client certificates.

The server-csr.json file is used to configure our server's certificate.
For example, indicating which domain names the certificate should be valid for.

The client-csr.json file is used to configure our client's certificate.
The CN field is especially important here because that the client's identity
(almost like their username).
This is the identity that we'll store their permissions under for authorisation.