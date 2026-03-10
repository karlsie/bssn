SELECT kks.*, sh.kategori_sektor, sh.nama_csirt
FROM public.kinerja_keamanan_siber kks
LEFT JOIN public.stakeholder sh ON kks.id_stakeholder = sh.id_stakeholder
