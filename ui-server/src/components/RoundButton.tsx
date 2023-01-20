import React from 'react';
import * as styles from './RoundButton.scss';

type RoundButtonProps = {
    img: string,
    tooltip?: string,
    onClick?: (e: MouseEvent) => void
}

export default function RoundButton({ img, tooltip = 'Tooltip', onClick = (e) => {} }: RoundButtonProps) {
    return <div className={styles.roundButton}>
        <button>
            <img src={ img } alt={ tooltip }></img>
        </button>
        <p>
            <span>{ tooltip }</span>
        </p>
    </div>
}