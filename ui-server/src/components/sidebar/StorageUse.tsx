import React from 'react';
import _ from 'lodash';
import * as styles from './StorageUse.scss';

import PercentageBar from '../PercentageBar';

type StorageUseProps = {
    used: number,
    total: number,
    unit: string
}

export default function StorageUse({ used, total, unit }: StorageUseProps) {
    return <div className={styles.storageUse}>
        <div className={styles.label}>
            <img src="images/icons/cloud.svg"></img><span>Storage</span>
        </div>
        <PercentageBar value={used / total} />
        <span className={styles.details}>{`${_.round(used, 2)}${unit} of ${_.round(total, 2)}${unit}`}</span>
    </div>
}