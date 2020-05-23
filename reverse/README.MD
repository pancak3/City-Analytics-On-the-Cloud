# Reverse proxy configuration

Reverse proxies one couch node and load balances app instances (with custom domain and HTTPs)  
This was hosted on an external IP VM on a MRC personal project  
Motivation: convenience, can access with VPN, VM can also be used as SSH jump host

Was not automated due to a lack of time

## Dependencies

- nginx: reverse proxy
- certbot: provisions LetsEncrypt HTTPs certificates
- apache2-utils: generates the htpasswd required to provide basic authentication for app subdomain