package com.ververica.flinktraining.exercises.datastream_java.datatypes;

import com.ververica.flinktraining.exercises.datastream_java.utils.GeoUtils;

public class EnrichedRide extends TaxiRide {
	public int startCell;
	public int endCell;

	public EnrichedRide() {
	}

	public EnrichedRide(TaxiRide ride) {
		this.rideId = ride.rideId;
		this.isStart = ride.isStart;

		this.startCell = GeoUtils.mapToGridCell(ride.startLon, ride.startLat);
		this.endCell = GeoUtils.mapToGridCell(ride.endLon, ride.endLat);
	}

	public String toString() {
		return super.toString() + "," + Integer.toString(this.startCell) + "," + Integer.toString(this.endCell) + ",aa";
	}

}
