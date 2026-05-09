package com.telemetry.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "vehicles")
@Getter @Setter
@NoArgsConstructor
public class Vehicle {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "vehicle_id", unique = true, nullable = false, length = 50)
    private String vehicleId;

    @Column(length = 100)
    private String name;

    @Column(length = 100)
    private String owner;

    @Column(nullable = false)
    private boolean active = true;

    @CreationTimestamp
    @Column(name = "registered_at", updatable = false)
    private LocalDateTime registeredAt;

    public Vehicle(String vehicleId, String name, String owner) {
        this.vehicleId = vehicleId;
        this.name = name;
        this.owner = owner;
    }
}
