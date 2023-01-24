import React from 'react';
import _ from 'lodash';
import * as styles from './PercentageBar.scss';

type PercentageBarProps = {
    value: number
}

export default function PercentageBar({ value }: PercentageBarProps) {
    if (value >= 0 && value <= 1) {
        value = _.round(value * 100);
    }
    else if (value < 0 || value > 100) {
        value = 0;
        console.error(`Invalid percentage value: ${value}`)
    }

    const remainder: number = 100-value;

    return <div className={styles.percentageBar}>
        <div style={{'width': `${value}%`}}></div>
        {
            remainder > 0 ?
                <div style={{'width': `${100-value}%`}}></div> :
                <></>
        }
    </div>
}