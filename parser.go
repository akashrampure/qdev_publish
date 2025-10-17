package main

import (
	"encoding/hex"
	"fmt"
	"strings"
)

type Device struct {
	DeviceID   string
	BLEChannel int
	RSSI       int
	Meta       []byte
}

type ParsedData struct {
	Lat       float64
	Lng       float64
	LatLngDir byte
	Devices   []Device
}

func ParseHexData(hexStr string) (*ParsedData, error) {
	data, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, err
	}

	if len(data) < 10 {
		return nil, fmt.Errorf("data too short")
	}

	p := &ParsedData{}

	rawLat := uint32(data[3])<<24 | uint32(data[2])<<16 | uint32(data[1])<<8 | uint32(data[0])
	rawLng := uint32(data[7])<<24 | uint32(data[6])<<16 | uint32(data[5])<<8 | uint32(data[4])

	p.LatLngDir = data[8]
	deviceCount := int(data[9])

	p.Lat = float64(rawLat) / 6000000.0
	p.Lng = float64(rawLng) / 6000000.0

	offset := 10
	for i := 0; i < deviceCount; i++ {
		if offset+11 > len(data) {
			return nil, fmt.Errorf("insufficient data for device %d", i)
		}
		dev := Device{}
		dev.DeviceID = strings.ToUpper(hex.EncodeToString(data[offset : offset+8]))
		dev.BLEChannel = int(data[offset+8])
		dev.RSSI = int(data[offset+9])
		metaLen := int(data[offset+10])
		offset += 11

		if offset+metaLen > len(data) {
			return nil, fmt.Errorf("insufficient data for device %d meta", i)
		}
		dev.Meta = data[offset : offset+metaLen]
		offset += metaLen

		p.Devices = append(p.Devices, dev)
	}

	return p, nil
}
