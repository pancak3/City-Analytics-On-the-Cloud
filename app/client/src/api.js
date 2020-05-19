import axios from 'axios';
if (process.env.URL === undefined) {
    console.warn('Warning: API URL undefined');
}
const BASE_URL = process.env.URL === '' ? '' : process.env.URL || 'http://localhost:3000';

export function getStats() {
    return axios.get(`${BASE_URL}/api/stats`).then((response) => {
        return response.data;
    });
}

export function getGeneralInfo() {
    return axios.get(`${BASE_URL}/api/general`).then((response) => {
        return response.data;
    });
}
