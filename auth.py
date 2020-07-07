import sys, json, requests, msal

# Cert based authentication using a service principal against MSFT AAD common endpoint (v2)
# This is using MSAL over ADAL and uses a private key with a thumbprint loaded onto the service principal for authentication
# TODO: provide instructions in README on how to get the .pem
def get_auth_token(paramFile):
    
    result = None
    auth = paramFile["authority_type"]

    if auth == "msi":
        result = json.loads(requests.get(paramFile["authority"] + "&resource=" + paramFile["resource"] + "&client_id=" + paramFile["client_id"], headers={"Metadata": "true"}).text)

    elif auth == "spn-cert" or auth == "spn-key":
        app = msal.ConfidentialClientApplication(
            paramFile["client_id"], authority=paramFile["authority"],
            client_credential=  {"thumbprint": paramFile["thumbprint"], "private_key": open(paramFile['private_key_file']).read()} if auth == "spn-cert" else paramFile["client_secret"]
        )
        result = app.acquire_token_for_client(scopes=[paramFile["resource"] + "/.default"])
        
    elif auth == "pat":
        result = {'access_token': paramFile["pat_token"]}

    else:
        # TODO: Raise exception
        result = ""
    return result