docker compose build -no--cache
docker compose up -d
docker logs -f dataflow360_spark_cleaner


- mmongosh -u mongo_admin -p mongo_admin123


Total patients en soin : TotalPatientsInCare = SUM('HospitalData'[patients_in_care])

Taux d’occupation lits (%) :
OccupancyRate = DIVIDE(SUM('HospitalData'[patients_in_care]), SUM('HospitalData'[capacity_beds]), 0)

Taux de disponibilité du personnel (%)
StaffAvailabilityRate = DIVIDE(SUM('HospitalData'[available_staff]), SUM('HospitalData'[capacity_staff]), 0)

Moyenne d’admissions par service : AvgAdmissions = AVERAGE('HospitalData'[patients_admitted])
Taux de rotation des lits (%) : BedTurnover = DIVIDE(SUM('HospitalData'[patients_released]), SUM('HospitalData'[capacity_beds]), 0)