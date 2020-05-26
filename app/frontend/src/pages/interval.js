// @author Team 42, Melbourne, Steven Tang, 832031

import { useEffect, useRef } from 'react';

// Setinterval for periodic polling
// https://overreacted.io/making-setinterval-declarative-with-react-hooks/

export default (callback, delay) => {
    const savedCallback = useRef();

    // Remember the latest callback.
    useEffect(() => {
        savedCallback.current = callback;
    }, [callback]);

    // Set up the interval.
    useEffect(() => {
        function tick() {
            savedCallback.current();
        }
        if (delay !== null) {
            let id = setInterval(tick, delay);
            return () => clearInterval(id);
        }

        return function cleanup() {
            clearInterval(savedCallback.current);
        };
    }, [delay]);
};
