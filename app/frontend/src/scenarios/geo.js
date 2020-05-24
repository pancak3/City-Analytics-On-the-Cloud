// Adds values to geojson
const prepareGeoJSON = (geojson, data) => {
    return geojson && data
        ? geojson.map((feature) => {
              return {
                  ...feature,
                  properties: {
                      ...feature.properties,
                      feature_value: data[feature.properties.feature_code],
                  },
              };
          })
        : null;
};
export { prepareGeoJSON };
