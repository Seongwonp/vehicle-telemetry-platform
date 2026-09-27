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

    /**
     * 소유자(users FK, V4). 응답에는 항상 username이 나가므로 조회 메서드가 {@code @EntityGraph}로 함께
     * 읽는다 — 트랜잭션 밖에서 {@code getOwnerUsername()}을 부르는 서비스가 있기 때문이다(VehicleService).
     */
    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "owner_id", nullable = false)
    private User owner;

    @Column(nullable = false)
    private boolean active = true;

    @CreationTimestamp
    @Column(name = "registered_at", updatable = false)
    private LocalDateTime registeredAt;

    public Vehicle(String vehicleId, String name, User owner) {
        this.vehicleId = vehicleId;
        this.name = name;
        this.owner = owner;
    }

    public String getOwnerUsername() {
        return owner == null ? null : owner.getUsername();
    }
}
