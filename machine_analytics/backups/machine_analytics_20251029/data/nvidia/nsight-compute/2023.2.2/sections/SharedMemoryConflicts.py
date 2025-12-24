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
    return "SharedMemoryConflicts"

def get_name():
    return "Shared Memory Conflicts"

def get_description():
    return "Detection of shared memory bank conflicts."

def get_section_identifier():
    return "MemoryWorkloadAnalysis_Tables"

def apply(handle):
    ctx = NvRules.get_context(handle)
    action = ctx.range_by_idx(0).action_by_idx(0)
    fe = ctx.frontend()

    shared_access_types = {
        "Shared Load"  : ["mem_shared_op_ld", "shared_ld"],
        "Shared Store" : ["mem_shared_op_st", "shared_st"]
    }

    cc_major = action.metric_by_name("device__attribute_compute_capability_major").as_uint64()
    cc_minor = action.metric_by_name("device__attribute_compute_capability_minor").as_uint64()
    cc = cc_major * 10 + cc_minor

    for access_info, metric_str in shared_access_types.items():
        requests = action.metric_by_name("smsp__sass_inst_executed_op_{}.sum".format(metric_str[1])).as_double()
        if (cc >= 75) and  (access_info == "Shared Load"):
            requests += action.metric_by_name("smsp__inst_executed_op_ldsm.sum").as_double()

        wavefronts_metric_name = "l1tex__data_pipe_lsu_wavefronts_{}.sum".format(metric_str[0])
        wavefronts = action.metric_by_name(wavefronts_metric_name).as_double()

        bank_conflicts_metric_name = "l1tex__data_bank_conflicts_pipe_lsu_{}.sum".format(metric_str[0])
        bank_conflicts = action.metric_by_name(bank_conflicts_metric_name).as_double()

        bank_conflicts_percent = (bank_conflicts*100.0)/wavefronts if wavefronts > 0 else 0.0
        bank_conflicts_threshold = 10.0

        if (bank_conflicts_percent >= bank_conflicts_threshold):
            message = "The memory access pattern for {}s might not be optimal ".format(access_info.lower())
            message += "and causes on average a {:.1f} - way bank conflict ".format(wavefronts/requests)
            message += "across all {:.0f} {} requests.".format(requests,  access_info.lower())
            message += "This results in {:.0f} bank conflicts, ".format(bank_conflicts)
            message += " which represent {:.2f}% ".format(bank_conflicts_percent)
            message += "of the overall {:.0f} wavefronts for {}s.".format(wavefronts, access_info.lower())
            message += " Check the @section:SourceCounters:Source Counters@ section for uncoalesced {}s.".format(access_info.lower())

            msg_id = fe.message(NvRules.IFrontend.MsgType_MSG_OPTIMIZATION, message, "{} Bank Conflicts".format(access_info))

            l1tex_active_cycles_name = "l1tex__cycles_active.sum"
            l1tex_all_cycles_name = "l1tex__cycles_elapsed.sum"
            if (l1tex_active_cycles_name in action) and (l1tex_all_cycles_name in action):
                l1tex_utilization = action[l1tex_active_cycles_name].value() / action[l1tex_all_cycles_name].value()
            else:
                l1tex_utilization = 0
            improvement_percent = bank_conflicts_percent * l1tex_utilization
            fe.speedup(msg_id, NvRules.IFrontend.SpeedupType_LOCAL, improvement_percent)

            fe.focus_metric(msg_id, \
                            bank_conflicts_metric_name, \
                            bank_conflicts, \
                            NvRules.IFrontend.Severity_SEVERITY_DEFAULT, \
                            "Decrease bank conflicts for {}s".format(access_info.lower()))
