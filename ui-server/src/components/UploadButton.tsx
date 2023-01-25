import React from 'react';
import * as styles from './UploadButton.scss';

type UploadButtonProps = {
    onClick?: (e: React.MouseEvent) => void
}

export default function UploadButton({ onClick = (e) => {} }: UploadButtonProps) {
    return <button className={styles.uploadButton} onClick={onClick}>
        <img src="images/icons/top-arrow.svg"></img>
        <span>Upload</span>
    </button>
}