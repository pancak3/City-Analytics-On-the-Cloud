import React, { useEffect, useState } from 'react';
import Scenario from '../components/Scenario';
import { getCounts } from '../helper/api';
import PropTypes from 'prop-types';

function Word(props) {
    const [counts, setCounts] = useState(null);
    const [loaded, setLoaded] = useState(false);

    useEffect(() => {
        if (loaded) return;

        setLoaded(true);
        getCounts().then((data) => {
            setCounts(data);
        });
    }, [loaded]);

    const geojson =
        props.geojson && counts
            ? props.geojson.map((feature) => {
                  return {
                      ...feature,
                      properties: {
                          ...feature.properties,
                          feature_value:
                              counts[feature.properties.feature_code],
                      },
                  };
              })
            : null;

    return <Scenario data={geojson} mode={'k'}></Scenario>;
}

Word.propTypes = {
    geojson: PropTypes.array,
};

export default Word;
