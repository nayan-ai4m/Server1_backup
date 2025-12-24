import NvRules


def get_identifier():
    return "TemplateRuleSpeedup"


def get_name():
    return "Speedup Estimation Template"


def get_description():
    return "A rule template containing a speedup estimation and focus metrics."


def apply(handle):
    ctx = NvRules.get_context(handle)
    action = ctx.range_by_idx(0).action_by_idx(0)
    frontend = ctx.frontend()

    # Get metrics for the speedup estimation
    compute_throughput_name = "sm__throughput.avg.pct_of_peak_sustained_elapsed"
    memory_throughput_name = (
        "gpu__compute_memory_throughput.avg.pct_of_peak_sustained_elapsed"
    )
    compute_throughput = action[compute_throughput_name].value()
    memory_throughput = action[memory_throughput_name].value()

    # Calculate the potential speedup; we will later need a number in percent
    if compute_throughput > memory_throughput:
        dominated_by = "compute"
        improvement_percent = 100 - compute_throughput
    else:
        dominated_by = "memory"
        improvement_percent = 100 - memory_throughput

    # Post a message to the frontend summarizing the outcome of the rule.
    # Messages of type OPTIMIZATION with a speedup estimate will also be displayed
    # on the summary page.
    message_id = frontend.message(
        NvRules.IFrontend.MsgType_MSG_OPTIMIZATION,
        "This kernel is currently dominated by {}.".format(dominated_by),
        "Compute vs Memory",
    )

    # Attach a speedup estimate to the last message.
    # Since we did not make sure that the required metrics are collected by a
    # parent-scope section, we ought to make sure that the metrics are collected manually
    # or by another section. We express this here with SpeedupType_GLOBAL.
    frontend.speedup(
        message_id, NvRules.IFrontend.SpeedupType_GLOBAL, improvement_percent
    )

    # Attach the two metrics which entered the speedup estimation to the last message.
    # These metrics serve as focus metrics or key performance indicators and should
    # be tracked when optimizing the kernel according to this rule.
    frontend.focus_metric(
        message_id,
        compute_throughput_name,
        compute_throughput,
        NvRules.IFrontend.Severity_SEVERITY_DEFAULT,
        "Increase the compute throughput",
    )
    frontend.focus_metric(
        message_id,
        memory_throughput_name,
        memory_throughput,
        NvRules.IFrontend.Severity_SEVERITY_DEFAULT,
        "Increase the memory throughput",
    )
