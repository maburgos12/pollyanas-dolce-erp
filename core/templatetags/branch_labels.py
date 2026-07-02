from django import template

from core.branch_catalog import display_branch, display_branch_name

register = template.Library()


@register.filter
def branch_label(branch):
    return display_branch(branch)


@register.filter
def branch_label_name(name):
    return display_branch_name(name)
