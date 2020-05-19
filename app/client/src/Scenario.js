import React, { useState } from 'react';
import { Map, TileLayer } from 'react-leaflet';
import Choropleth from 'react-leaflet-choropleth';
import data from './MelbourneGeojson';

class Scenario extends React.Component {
    render() {
        const props = this.props;

        let state = {
            lat: -37.8,
            lng: 145.7,
            zoom: 9,
        };
        const position = [state.lat, state.lng];

        return (
            <React.Fragment>
                <Map id="map" center={position} zoom={state.zoom}>
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
    }
}
export default Scenario;
