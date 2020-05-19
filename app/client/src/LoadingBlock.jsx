import React from 'react';
import PropTypes from 'prop-types';

// A loading block
class LoadingBlock extends React.Component {
    render() {
        return (
            <div
                className={'loading'}>
                {this.props.children}
            </div>
        );
    }
}

// Define props
LoadingBlock.propTypes = {
    children: PropTypes.element.isRequired
};
export default LoadingBlock;
