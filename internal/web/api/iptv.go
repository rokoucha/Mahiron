package api

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/21S1298001/Mahiron5/internal/program"
	"github.com/21S1298001/Mahiron5/internal/server/middleware"
	"github.com/21S1298001/Mahiron5/internal/service"
	apigen "github.com/21S1298001/Mahiron5/internal/web/api/gen"
)

const (
	iptvName            = "Mahiron5"
	iptvFirmwareVersion = "5.0.0"
	iptvDeviceID        = "12345678"
	iptvDeviceAuth      = "mahiron5"
)

func IptvDiscoverJSONGet(ctx context.Context, h *Handler) (apigen.IptvDiscoverJSONGetRes, error) {
	services, err := h.serviceManager.GetServices(ctx)
	if err != nil {
		return nil, err
	}
	baseURL := iptvBaseURL(ctx)
	return &apigen.IptvDiscover{
		FriendlyName:    iptvName,
		Manufacturer:    iptvName,
		ModelNumber:     iptvName,
		FirmwareName:    iptvName,
		TunerCount:      len(services),
		FirmwareVersion: iptvFirmwareVersion,
		DeviceID:        iptvDeviceID,
		DeviceAuth:      iptvDeviceAuth,
		BaseURL:         baseURL,
		LineupURL:       baseURL + "/iptv/lineup.json",
	}, nil
}

func IptvLineupJSONGet(ctx context.Context, h *Handler) (apigen.IptvLineupJSONGetRes, error) {
	services, err := h.serviceManager.GetServices(ctx)
	if err != nil {
		return nil, err
	}
	items := make(apigen.IptvLineupJSONGetOKApplicationJSON, len(services))
	baseURL := iptvBaseURL(ctx)
	for i, svc := range services {
		items[i] = apigen.IptvLineupItem{
			GuideNumber: iptvGuideID(svc),
			GuideName:   svc.Name,
			URL:         iptvServiceStreamURL(baseURL, svc),
		}
	}
	return &items, nil
}

func IptvLineupStatusJSONGet(ctx context.Context, h *Handler) (apigen.IptvLineupStatusJSONGetRes, error) {
	return &apigen.IptvLineupStatus{
		ScanInProgress: 0,
		ScanPossible:   1,
		Source:         "Cable",
		SourceList:     []string{"Cable"},
	}, nil
}

func IptvPlaylistGet(ctx context.Context, h *Handler) (apigen.IptvPlaylistGetRes, error) {
	services, err := h.serviceManager.GetServices(ctx)
	if err != nil {
		return nil, err
	}
	baseURL := iptvBaseURL(ctx)
	var b strings.Builder
	fmt.Fprintf(&b, "#EXTM3U x-tvg-url=\"%s\"\n", m3uAttrEscape(baseURL+"/iptv/xmltv"))
	for _, svc := range services {
		guideID := iptvGuideID(svc)
		channelName := svc.ChannelType
		if channel := h.serviceManager.GetChannel(svc.ChannelType, svc.ChannelId); channel != nil && channel.Name != "" {
			channelName = channel.Name
		}
		chno := guideID
		if svc.RemoteControlKeyId != 0 {
			chno = strconv.Itoa(int(svc.RemoteControlKeyId))
		}
		fmt.Fprintf(
			&b,
			"#EXTINF:-1 tvg-id=\"%s\" tvg-name=\"%s\" channel-id=\"%s\" group-title=\"%s\" tvg-chno=\"%s\",%s\n",
			m3uAttrEscape(guideID),
			m3uAttrEscape(svc.Name),
			m3uAttrEscape(guideID),
			m3uAttrEscape(channelName),
			m3uAttrEscape(chno),
			m3uTextEscape(svc.Name),
		)
		fmt.Fprintf(&b, "%s\n", iptvServiceStreamURL(baseURL, svc))
	}
	return &apigen.IptvPlaylistGetOK{Data: strings.NewReader(b.String())}, nil
}

func IptvXmltvGet(ctx context.Context, h *Handler) (apigen.IptvXmltvGetRes, error) {
	services, err := h.serviceManager.GetServices(ctx)
	if err != nil {
		return nil, err
	}
	programs, err := h.programManager.List(ctx, program.Query{})
	if err != nil {
		return nil, err
	}
	data, err := buildXMLTV(services, programs)
	if err != nil {
		return nil, err
	}
	return &apigen.IptvXmltvGetOK{Data: bytes.NewReader(data)}, nil
}

func iptvBaseURL(ctx context.Context) string {
	if request, err := middleware.GetRequestInfo(ctx); err == nil && request.Host != "" {
		scheme := request.Scheme
		if scheme == "" {
			scheme = "http"
		}
		return scheme + "://" + request.Host + "/api"
	}
	return "http://localhost:40772/api"
}

func iptvGuideID(svc *service.Service) string {
	return strconv.FormatInt(svc.ItemId(), 10)
}

func iptvServiceStreamURL(baseURL string, svc *service.Service) string {
	return fmt.Sprintf("%s/services/%d/stream?decode=1", baseURL, svc.ItemId())
}

func m3uAttrEscape(s string) string {
	replacer := strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\r", " ", "\n", " ")
	return replacer.Replace(s)
}

func m3uTextEscape(s string) string {
	replacer := strings.NewReplacer("\r", " ", "\n", " ")
	return replacer.Replace(s)
}

type xmltvDocument struct {
	XMLName  xml.Name       `xml:"tv"`
	Source   string         `xml:"source-info-name,attr,omitempty"`
	Channels []xmltvChannel `xml:"channel"`
	Programs []xmltvProgram `xml:"programme"`
}

type xmltvChannel struct {
	ID          string          `xml:"id,attr"`
	DisplayName []xmltvTextNode `xml:"display-name"`
}

type xmltvProgram struct {
	Start    string          `xml:"start,attr"`
	Stop     string          `xml:"stop,attr"`
	Channel  string          `xml:"channel,attr"`
	Title    []xmltvTextNode `xml:"title"`
	Desc     []xmltvTextNode `xml:"desc,omitempty"`
	Category []xmltvTextNode `xml:"category,omitempty"`
}

type xmltvTextNode struct {
	Lang  string `xml:"lang,attr,omitempty"`
	Value string `xml:",chardata"`
}

func buildXMLTV(services []*service.Service, programs []*program.Program) ([]byte, error) {
	serviceNames := make(map[string]string, len(services))
	channels := make([]xmltvChannel, 0, len(services))
	for _, svc := range services {
		guideID := iptvGuideID(svc)
		serviceNames[guideID] = svc.Name
		channels = append(channels, xmltvChannel{
			ID: guideID,
			DisplayName: []xmltvTextNode{
				{Value: svc.Name},
			},
		})
	}

	xmlPrograms := make([]xmltvProgram, 0, len(programs))
	for _, p := range programs {
		channelID := iptvProgramGuideID(p)
		title := p.Name
		if title == "" {
			title = serviceNames[channelID]
		}
		if title == "" {
			title = "No Title"
		}

		item := xmltvProgram{
			Start:   xmltvTime(p.StartAt),
			Stop:    xmltvTime(p.StartAt + int64(p.Duration)),
			Channel: channelID,
			Title:   []xmltvTextNode{{Value: title}},
		}
		if p.Description != "" {
			item.Desc = []xmltvTextNode{{Value: p.Description}}
		}
		item.Category = xmltvCategories(p.Genres)
		xmlPrograms = append(xmlPrograms, item)
	}

	doc := xmltvDocument{
		Source:   iptvName,
		Channels: channels,
		Programs: xmlPrograms,
	}
	var b bytes.Buffer
	b.WriteString(xml.Header)
	encoder := xml.NewEncoder(&b)
	encoder.Indent("", "  ")
	if err := encoder.Encode(doc); err != nil {
		return nil, err
	}
	if err := encoder.Flush(); err != nil {
		return nil, err
	}
	b.WriteByte('\n')
	return b.Bytes(), nil
}

func xmltvTime(ms int64) string {
	return time.UnixMilli(ms).Format("20060102150405 -0700")
}

func iptvProgramGuideID(p *program.Program) string {
	return strconv.FormatInt(int64(p.NetworkID)*100000+int64(p.ServiceID), 10)
}

func xmltvCategories(genres []program.Genre) []xmltvTextNode {
	if len(genres) == 0 {
		return nil
	}
	categories := make([]xmltvTextNode, len(genres))
	for i, genre := range genres {
		categories[i] = xmltvTextNode{Value: fmt.Sprintf("%d/%d", genre.Lv1, genre.Lv2)}
	}
	return categories
}

var _ io.Reader = (*apigen.IptvPlaylistGetOK)(nil)
var _ io.Reader = (*apigen.IptvXmltvGetOK)(nil)
