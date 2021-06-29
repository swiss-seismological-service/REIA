export async function getData(endpoint) {
    const response = fetch(`/api/v1${endpoint}`)
        .then((resp) => {
            if (!resp.ok) throw Error(resp.statusText);
            return resp.json();
        })
        .then((json) => json);
    return response;
}

export async function postExposure(files) {
    const formData = new FormData();
    formData.append('exposureJSON', files.exposureJSON[0]);
    formData.append('exposureCSV', files.exposureCSV[0]);

    const response = fetch('/api/v1/exposure', {
        method: 'POST',
        body: formData,
    })
        .then((resp) => {
            if (!resp.ok) throw Error(resp.statusText);
            return resp.json();
        })
        .then((json) => json);
    return response;
}

export async function postVulnerability(files) {
    const formData = new FormData();
    formData.append('vulnerabilitymodel', files[0]);

    const response = fetch('/api/v1/vulnerability', {
        method: 'POST',
        body: formData,
    })
        .then((resp) => {
            if (!resp.ok) throw Error(resp.statusText);
            return resp.json();
        })
        .then((json) => json);
    return response;
}

export async function postLossModel(values) {
    const formData = new FormData();
    formData.append('riskini', values.modelJson[0]);
    formData.append('_assetcollection_oid', values.assetCollectionId);
    formData.append('_vulnerabilitymodels_oids', values.vulnerabilityModelIds);
    const response = fetch('/api/v1/lossmodel', {
        method: 'POST',
        body: formData,
    })
        .then((resp) => {
            if (!resp.ok) throw Error(resp.statusText);
            return resp.json();
        })
        .then((json) => json);
    return response;
}

export async function postLossConfig(data) {
    const response = fetch('/api/v1/lossconfig', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(data),
    })
        .then((resp) => {
            if (!resp.ok) throw Error(resp.statusText);
            return resp.json();
        })
        .then((json) => json);
    return response;
}
