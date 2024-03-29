(ns precipitation
  {:nextjournal.clerk/visibility {:code :show :result :show}}
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [hato.client :as http]
            [hato.middleware :as hm]
            [tablecloth.api :as tc]
            [nextjournal.clerk :as clerk])
  (:import (java.time Instant LocalDateTime LocalDate)
           (java.time.format DateTimeFormatter)
           (java.time.temporal ChronoUnit ChronoField)))


(defmethod hm/coerce-form-params :application/json
  [{:keys [form-params _json-opts]}]
  (json/write-str form-params))

(defmethod hm/coerce-response-body :json
  [_req {:keys [body] :as resp}]
  (assoc resp :body (json/read (io/reader body)
                               {:key-fn keyword})))


^::clerk/sync
(defonce location
  (atom {:lat "48.2084828"
         :lng "16.3731725"}))

(clerk/html
 [:link {:rel "stylesheet"
         :href "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
         :crossorigin ""}])

(def leaflet
  {:transform-fn clerk/mark-presented
   :render-fn
   '(fn [value]
      (nextjournal.clerk.viewer/html
       (when-let [{:keys [lat lng res-lat res-lng]} value]
         [nextjournal.clerk.viewer/with-d3-require {:package ["leaflet@1.9.4/dist/leaflet.js"]}
          (fn [leaflet]
            [:div {:style {:height 400}
                   :ref
                   (fn [el]
                     (when el
                       (when-let [m (.-map el)] (.remove m))
                       (let [m                   (.map leaflet el (clj->js {:zoomControl true :zoomDelta 2  :zoomSnap 0.5 :attributionControl false}))
                             location-latlng     (.latLng leaflet lat lng)
                             rr-bounds           (.toBounds (.latLng leaflet res-lat res-lng) 1000)
                             location-marker     (.marker leaflet location-latlng)
                             basemap-hidpi-layer (.tileLayer leaflet
                                                             "https://{s}.wien.gv.at/basemap/bmaphidpi/normal/google3857/{z}/{y}/{x}.jpeg"
                                                             (clj->js
                                                              {:subdomains    ["maps" "maps1" "maps2" "maps3" "maps4"]
                                                               :maxZoom       25
                                                               :maxNativeZoom 19
                                                               :attribution   "basemap.at"
                                                               :errorTileUrl  "/transparent.gif"}))]
                         (set! (.-map el) m)
                         (.on m "click"
                          (fn map-on-click [event]
                            (nextjournal.clerk.viewer/clerk-eval
                                         `(reset! location {:lat  ~(.. event -latlng -lat) :lng ~(.. event -latlng -lng)}))))
                         (.addTo basemap-hidpi-layer m)
                         (.addTo location-marker m)
                         (.addTo (.rectangle leaflet rr-bounds (clj->js {:color "#ff7800" :weight 1})) m)
                         (.setView m location-latlng 13.7))))}])])))})

(def precipitation-for
  {:duration (* 10 365)
   :unit     ChronoUnit/DAYS})

(defn rain-time-series* [from to {:keys [lat lng]} ]
  (http/get (str  "https://dataset.api.hub.geosphere.at/v1/timeseries/historical/inca-v1-1h-1km"
                  "?parameters=RR"
                  "&start=" from
                  "&end=" to
                  "&lat_lon=" lat "," lng)
            {:as :json}))

(defonce rain-time-series (memoize rain-time-series*))

^::clerk/no-cache
 (def r
 (let [date (LocalDate/now)]
   (rain-time-series
    (.minus date (:duration precipitation-for) (:unit precipitation-for))
    date
    @location)))

(comment
  (get-in r [:body :timestamps])
  (get-in r [:body :features 0 :properties :parameters :RR]))

(def res-latlng
  (let [[lng lat] (get-in r [:body :features 0 :geometry :coordinates])]
    {:res-lat lat
     :res-lng lng}))

(def dtf DateTimeFormatter/ISO_OFFSET_DATE_TIME)

(def ds (-> (tc/dataset [[:timestamp (get-in r [:body :timestamps])]
                         [:RR (get-in r [:body :features 0 :properties :parameters :RR :data])]])
            ;; some values are nil
            (tc/replace-missing :RR :value 0)
            (tc/map-rows (fn [row] (update row :timestamp #(LocalDateTime/from (.parse dtf %)))))))


;; Click the map to calculate precipitation statistics for a different spot
(clerk/with-viewer leaflet
  (merge @location
         res-latlng))


(tc/aggregate ds #(reduce + (:RR %)))


(def ds-by-year
  (-> ds
      (tc/group-by (fn [row] (.get (:timestamp row) ChronoField/YEAR)) {:name "year"})
      (tc/aggregate #(reduce + (:RR %)))
      (tc/rename-columns {:$group-name "year"
                          "summary" "RR"})))

(clerk/table
 (tc/rows ds-by-year :as-maps))

{:nextjournal.clerk/visibility {:code :hide :result :hide}}

(comment
  (clerk/serve! {:browse? true})
  )
