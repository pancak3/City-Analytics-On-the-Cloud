// @author Team 42, Melbourne, Steven Tang, 832031

import React from 'react';
import PropTypes from 'prop-types';

// A loading block
class LoadingBlock extends React.Component {
    render() {
        return <span className={'loading'}>{this.props.children}</span>;
    }
}

// Define props
LoadingBlock.propTypes = {
    children: PropTypes.any,
};
export default LoadingBlock;
