package systems.systemD.dataclasses

/**
 * Simple data class representing one employee with optional information about an IAV certificate.
 * If the employee has a IAVCertificate, the flag "hasCert" is set to true.
 * If the employee doesn't have a certificate, the "certId" field is set to -1.
 *
 * @author Felix Eder
 * @date 2021-05-02
 */
data class Employee(val personalNumber: String, val hasCert: Boolean, val certId: Int)
