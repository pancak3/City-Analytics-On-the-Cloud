// @author Team 42, Melbourne, Steven Tang, 832031

import React, { useState } from 'react';
import { Map, TileLayer, Marker } from 'react-leaflet';
import Choropleth from '../react-leaflet-choropleth/choropleth';
import PropTypes from 'prop-types';
import { victoria } from '../helper/latlong';

// https://github.com/PaulLeCam/react-leaflet/issues/453
import 'leaflet/dist/leaflet.css';
import L from 'leaflet';

delete L.Icon.Default.prototype._getIconUrl;

L.Icon.Default.mergeOptions({
    iconRetinaUrl: require('leaflet/dist/images/marker-icon-2x.png'),
    iconUrl: require('leaflet/dist/images/marker-icon.png'),
    shadowUrl: require('leaflet/dist/images/marker-shadow.png'),
});

const Scenario = (props) => {
    const { marker } = props;

    const [pos_zoom] = useState(props.position || victoria);
    const position = [pos_zoom.lat, pos_zoom.lng];
    // adapted from https://blog.datawrapper.de
    const scale = props.scale || [
        '#f3e9cd',
        '#d2e3c8',
        '#90d2a2',
        '#69c595',
        '#82c4af',
        '#6b9ba9',
        '#00326e',
    ];
    const steps = props.steps || 5;

    const style = {
        weight: 0.5,
        opacity: 1,
        color: '#444',
        dashArray: '3',
        fillOpacity: 0.8,
    };

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
                        features: props.data || [],
                    }}
                    valueProperty={(feature) => {
                        return Number(feature.properties.feature_value) || 0;
                    }}
                    scale={scale}
                    steps={steps}
                    mode={props.mode || 'e'}
                    // onEachFeature={(feature, layer) => {
                    // layer.bindPopup(feature.properties.feature_name)
                    // }
                    featureClick={props.featureClick}
                    style={style}
                />

                {marker ? <Marker position={marker} /> : <React.Fragment />}
            </Map>
            <div id="info">{props.children}</div>
        </React.Fragment>
    );
};

Scenario.propTypes = {
    children: PropTypes.any,
    position: PropTypes.object,
    data: PropTypes.array,
    scale: PropTypes.array,
    steps: PropTypes.number,
    mode: PropTypes.string,
    featureClick: PropTypes.func,
    marker: PropTypes.array,
};

export default Scenario;
