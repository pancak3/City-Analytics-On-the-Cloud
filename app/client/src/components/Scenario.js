import React, { useState } from 'react';
import { Map, TileLayer } from 'react-leaflet';
import Choropleth from 'react-leaflet-choropleth';
import data from './MelbourneGeojson';
import PropTypes from 'prop-types';
import { victoria } from '../helper/latlong';

const Scenario = (props) => {
    const [pos_zoom] = useState(props.position || victoria);
    const position = [pos_zoom.lat, pos_zoom.lng];

    return (
        <React.Fragment>
            <Map id="map" center={position} zoom={pos_zoom.zoom}>
                <TileLayer
                    attribution='&amp;copy <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
                    url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                />

                <Choropleth
                    data={{
                        type: 'FeatureCollection',
                        features: data.features,
                    }}
                    valueProperty={(feature) => 0}
                    scale={['#b3cde0', '#011f4b']}
                    steps={7}
                    mode="e"
                    onEachFeature={(feature, layer) =>
                        layer.bindPopup(feature.properties.label)
                    }
                />
            </Map>
            <div id="info">{props.children}</div>
        </React.Fragment>
    );
};

Scenario.propTypes = {
    children: PropTypes.array,
    position: PropTypes.object.isRequired,
};

export default Scenario;
