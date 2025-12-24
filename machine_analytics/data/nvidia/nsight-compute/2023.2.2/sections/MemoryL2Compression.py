# Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#  * Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#  * Neither the name of NVIDIA CORPORATION nor the names of its
#    contributors may be used to endorse or promote products derived
#    from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS ``AS IS'' AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
# PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
# OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
import NvRules

def get_identifier():
    return "MemoryL2Compression"

def get_name():
    return "Memory L2 Compression"

def get_description():
    return "Detection of inefficient use of L2 Compute Data Compression"

def get_section_identifier():
    return "MemoryWorkloadAnalysis_Chart"


def get_estimated_speedup(action):
    compression_success_rate = action["lts__average_gcomp_input_sector_success_rate.pct"].value()
    compression_input_sectors = action["lts__gcomp_input_sectors.sum"].value()
    dram_bandwidth_percent = action["dram__bytes_read.sum.pct_of_peak_sustained_elapsed"].value()

    input_sectors_name = "lts__t_sectors.sum"
    if input_sectors_name in action:
        input_sectors = action[input_sectors_name].value()
    else:
        return NvRules.IFrontend.SpeedupType_LOCAL, 0

    improvement = (1 - compression_success_rate) * (1 - compression_input_sectors / input_sectors) * (dram_bandwidth_percent / 100)

    return NvRules.IFrontend.SpeedupType_LOCAL, improvement * 100


def apply(handle):
    ctx = NvRules.get_context(handle)
    action = ctx.range_by_idx(0).action_by_idx(0)
    fe = ctx.frontend()

    cc_major = action.metric_by_name("device__attribute_compute_capability_major").as_uint64()
    cc_minor = action.metric_by_name("device__attribute_compute_capability_minor").as_uint64()
    cc = cc_major * 10 + cc_minor

    low_compression_threshold = 20

    if 90 <= cc:
        return

    if 80 <= cc:
        compression_input_sectors_name = "lts__gcomp_input_sectors.sum"
        compression_input_sectors = action[compression_input_sectors_name].value()

        if 0.0 < compression_input_sectors:
            compInputBytes = 32.0 * compression_input_sectors
            compression_success_name = "lts__average_gcomp_input_sector_success_rate.pct"
            compression_success_rate = action[compression_success_name].value()

            input_sectors_name = "lts__t_sectors.sum"
            if input_sectors_name in action:
                input_sectors = action[input_sectors_name].value()
            else:
                input_sectors = 0

            # Check for low success rate
            if compression_success_rate < low_compression_threshold:
                message = "Out of the {:.1f} bytes sent to the L2 Compression unit only {:.2f}% were successfully compressed. To increase this success rate, consider marking only those memory regions as compressible that contain the most zero values and/or expose the most homogeneous values.".format(compInputBytes, compression_success_rate)
                msg_id = fe.message(NvRules.IFrontend.MsgType_MSG_OPTIMIZATION, message, "Low Compression Rate")

                speedup_type, speedup_value = get_estimated_speedup(action)
                fe.speedup(msg_id, speedup_type, speedup_value)

                fe.focus_metric(msg_id, compression_success_name, compression_success_rate, NvRules.IFrontend.Severity_SEVERITY_HIGH, "Increase the compression success rate")
                if input_sectors > 0:
                    fe.focus_metric(msg_id, compression_input_sectors_name, compression_input_sectors, NvRules.IFrontend.Severity_SEVERITY_HIGH,
                        "Increase the number of compressed sectors towards all L2 input sectors ({:,.0f})".format(input_sectors))
                fe.focus_metric(msg_id, "dram__bytes_read.sum.pct_of_peak_sustained_elapsed", action["dram__bytes_read.sum.pct_of_peak_sustained_elapsed"].value(), NvRules.IFrontend.Severity_SEVERITY_LOW,
                    "The higher the DRAM peak read utilization the more can be gained by compression")

            l1WriteBytes = action.metric_by_name("l1tex__m_l1tex2xbar_write_bytes.sum").as_double()
            # Check for write access pattern
            if (l1WriteBytes > 0) and (4.0 * l1WriteBytes < compInputBytes):
                message = "The access patterns for writes to compressible memory are not well suited for the L2 Compression unit. As a consequence, {:.1f}x the data written to the L2 cache has to be communicated to the L2 Compression unit. Try maximizing local coherence for the write operations to compressible memory. For example, avoid writes with large strides as they lead to partial accesses to many L2 cache lines. Instead, try accessing fewer overall cache lines by modifying many values per cache line with each warp's execution of a write operation.".format(compInputBytes/l1WriteBytes)
                msg_id = fe.message(NvRules.IFrontend.MsgType_MSG_OPTIMIZATION, message, "Access Pattern")

                speedup_type, speedup_value = get_estimated_speedup(action)
                fe.speedup(msg_id, speedup_type, speedup_value)

                fe.focus_metric(msg_id, compression_success_name, compression_success_rate, NvRules.IFrontend.Severity_SEVERITY_HIGH, "Increase the compression success rate")
                if input_sectors > 0:
                    fe.focus_metric(msg_id, compression_input_sectors_name, compression_input_sectors, NvRules.IFrontend.Severity_SEVERITY_HIGH,
                        "Increase the number of compressed sectors towards all L2 input sectors ({:,.0f})".format(input_sectors))
                fe.focus_metric(msg_id, "dram__bytes_read.sum.pct_of_peak_sustained_elapsed", action["dram__bytes_read.sum.pct_of_peak_sustained_elapsed"].value(), NvRules.IFrontend.Severity_SEVERITY_LOW,
                    "The higher the DRAM peak read utilization the more can be gained by compression")
