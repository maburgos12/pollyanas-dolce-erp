from rest_framework import serializers


class SpecialHoursPreviewSerializer(serializers.Serializer):
    text = serializers.CharField()
    reason = serializers.CharField(required=False, allow_blank=True, default="")
    source_channel = serializers.ChoiceField(
        choices=["WEB", "API", "IA_PRIVADA"],
        required=False,
        default="API",
    )


class SpecialHoursActionSerializer(serializers.Serializer):
    comment = serializers.CharField(required=False, allow_blank=True, default="")
    async_execute = serializers.BooleanField(required=False, default=True)

