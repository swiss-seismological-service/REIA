export async function getUsers(endpoint) {
  const response = fetch(endpoint)
    .then((response) => {
        if (!response.ok) throw Error(response.statusText);
        return response.json();
    })
    .then((json) => json);
    return response;
}

export async function uploadFile(files, data, endpoint) {
  
    var formData = new FormData();
    
    [...files].map((file, index) => {
     formData.append(`file${index}`, file);
    });

    formData.append('data', data);

    const response = fetch(endpoint, {
      method: 'POST',
      body: formData,
    })
    .then((response) => {
        if (!response.ok) throw Error(response.statusText);
        return response.json();
    })
    .then((json) => json);
    return response;
  }