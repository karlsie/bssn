SELECT kks.*, sh.kategori_sektor, sh.nama_csirt
FROM public.kinerja_keamanan_siber_dst kks
LEFT JOIN public.stakeholder_dst sh ON kks.id_stakeholder = sh.id_stakeholder
