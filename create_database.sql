-- phpMyAdmin SQL Dump
-- version 5.1.3
-- https://www.phpmyadmin.net/
--
-- Host: localhost
-- Generation Time: May 20, 2022 at 01:00 AM
-- Server version: 10.4.21-MariaDB
-- PHP Version: 7.4.29

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `bi_proyecto_semestral`
--

-- --------------------------------------------------------

--
-- Table structure for table `dim_estacion`
--

CREATE TABLE `dim_estacion` (
  `ID_ESTACION` int(11) NOT NULL,
  `NOMBRE` varchar(100) DEFAULT NULL,
  `LATITUD` float(20) DEFAULT NULL,
  `ALTITUD` float(20) DEFAULT NULL,
  `VIGENTE` boolean DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- --------------------------------------------------------

--
-- Table structure for table `dim_periodo`
--

CREATE TABLE `dim_periodo` (
  `ID_PERIODO` int(11) NOT NULL,
  `ANNIO` int(4) DEFAULT NULL,
  `MES` int(2) DEFAULT NULL,
  `VIGENTE` boolean DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- --------------------------------------------------------

--
-- Table structure for table `dim_region`
--

CREATE TABLE `dim_region` (
  `ID_REGION` int(11) NOT NULL,
  `NOMBRE_REGION` varchar(100) DEFAULT NULL,
  `VIGENTE` boolean DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- --------------------------------------------------------

--
-- Table structure for table `fact_temprec`
--

CREATE TABLE `fact_temprec` (
  `ID_TEMPREC` int(11) NOT NULL,
  `ID_PERIODO` int(11) NOT NULL,
  `ID_ESTACION` int(11) NOT NULL,
  `ID_REGION` int(11) NOT NULL,
  `MINIMA_TEMPERATURA_MAXIMA` float NOT NULL,
  `MAXIMA_TEMPERATURA_MAXIMA` float NOT NULL,
  `MINIMA_TEMPERATURA_MINIMA` float NOT NULL,
  `MAXIMA_TEMPERATURA_MINIMA` float NOT NULL,
  `PROMEDIO_TEMPERATURA_MINIMA` float NOT NULL,
  `PROMEDIO_TEMPERATURA_MAXIMA` float NOT NULL,
  `SUMA_PRECIPITACION` float NOT NULL,
  `PROMEDIO_PRECIPITACION` float NOT NULL,
  `PRECIPITACION_MAXIMA` float NOT NULL,
  `PRECIPITACION_MINIMA` float NOT NULL,
  `VIGENTE` boolean DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `dim_estacion`
--
ALTER TABLE `dim_estacion`
  ADD PRIMARY KEY (`ID_ESTACION`);

--
-- Indexes for table `dim_periodo`
--
ALTER TABLE `dim_periodo`
  ADD PRIMARY KEY (`ID_PERIODO`);

--
-- Indexes for table `dim_region`
--
ALTER TABLE `dim_region`
  ADD PRIMARY KEY (`ID_REGION`);

--
-- Indexes for table `fact_temprec`
--
ALTER TABLE `fact_temprec`
  ADD PRIMARY KEY (`ID_TEMPREC`),
  ADD KEY `ID_PERIODO` (`ID_PERIODO`),
  ADD KEY `ID_ESTACION` (`ID_ESTACION`),
  ADD KEY `ID_REGION` (`ID_REGION`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `dim_estacion`
--
ALTER TABLE `dim_estacion`
  MODIFY `ID_ESTACION` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `dim_periodo`
--
ALTER TABLE `dim_periodo`
  MODIFY `ID_PERIODO` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `dim_region`
--
ALTER TABLE `dim_region`
  MODIFY `ID_REGION` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `fact_temprec`
--
ALTER TABLE `fact_temprec`
  MODIFY `ID_TEMPREC` int(11) NOT NULL AUTO_INCREMENT;

--
-- Constraints for dumped tables
--

--
-- Constraints for table `fact_temprec`
--
ALTER TABLE `fact_temprec`
  ADD CONSTRAINT `fact_temprec_ibfk_1` FOREIGN KEY (`ID_PERIODO`) REFERENCES `dim_periodo` (`ID_PERIODO`),
  ADD CONSTRAINT `fact_temprec_ibfk_2` FOREIGN KEY (`ID_ESTACION`) REFERENCES `dim_estacion` (`ID_ESTACION`),
  ADD CONSTRAINT `fact_temprec_ibfk_3` FOREIGN KEY (`ID_REGION`) REFERENCES `dim_region` (`ID_REGION`);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;