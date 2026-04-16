from django.urls import path

from . import chat_views, views

app_name = "orquestacion"

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("chat/", chat_views.chat_home, name="chat_home"),
    path("chat/api/conversations/", chat_views.conversations_api, name="chat_conversations_api"),
    path("chat/api/conversations/new/", chat_views.create_conversation_api, name="chat_conversation_create_api"),
    path(
        "chat/api/conversations/<uuid:conversation_id>/",
        chat_views.conversation_detail_api,
        name="chat_conversation_detail_api",
    ),
    path(
        "chat/api/conversations/<uuid:conversation_id>/stream/",
        chat_views.stream_message_api,
        name="chat_message_stream_api",
    ),
    path("memory/", views.memory_proposals, name="memory_proposals"),
    path("memory/<int:proposal_id>/", views.memory_proposal_detail, name="memory_proposal_detail"),
    path("quality/", views.quality_findings, name="quality_findings"),
    path("quality/<int:finding_id>/", views.quality_finding_detail, name="quality_finding_detail"),
]
