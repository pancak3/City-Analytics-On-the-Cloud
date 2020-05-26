// @author Team 42, Melbourne, Steven Tang, 832031

import React from 'react';
import PropTypes from 'prop-types';

class Card extends React.Component {
    render() {
        const className = `card ${this.props.className}`;
        return <div className={className}>{this.props.children}</div>;
    }
}

Card.propTypes = {
    children: PropTypes.array,
    width: PropTypes.string,
    height: PropTypes.string,
    style: PropTypes.object,
    className: PropTypes.string,
};

export default Card;
