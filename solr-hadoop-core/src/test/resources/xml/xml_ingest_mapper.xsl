<?xml version="1.0"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:template match="/root">
    <top>
      <xsl:apply-templates select="@*|node()"/>
    </top>
  </xsl:template>
  <xsl:template match="dok">
    <doc>
      <xsl:apply-templates select="@*|node()"/>
    </doc>
  </xsl:template>
  <xsl:template match="@*|node()">
    <xsl:copy>
      <xsl:apply-templates select="@*|node()"/>
    </xsl:copy>
  </xsl:template>
</xsl:stylesheet>