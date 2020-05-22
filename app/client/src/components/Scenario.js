import React, { useState } from 'react';
import { Map, TileLayer } from 'react-leaflet';
import Choropleth from '../react-leaflet-choropleth/choropleth';
import PropTypes from 'prop-types';
import { victoria } from '../helper/latlong';

const Scenario = (props) => {
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
        weight: 2,
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
                    // onEachFeature={(feature, layer) =>
                    //     layer.bindPopup(feature.properties.feature_name)
                    // }
                    style={style}
                />
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
};

export default Scenario;
